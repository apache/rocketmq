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
import java.util.Map;

import org.apache.rocketmq.common.TopicQueueMappingInfo;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class TopicRouteData extends RemotingSerializable {
    private String orderTopicConf;
    private List<QueueData> queueDatas;
    private List<BrokerData> brokerDatas;
    private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;
    private Map<String, TopicQueueMappingInfo> topicQueueMappingByBroker;

    public TopicRouteData() {
    }

    public TopicRouteData(TopicRouteData topicRouteData) {
        this.queueDatas = new ArrayList<QueueData>();
        this.brokerDatas = new ArrayList<BrokerData>();
        this.filterServerTable = new HashMap<String, List<String>>();
        this.orderTopicConf = topicRouteData.orderTopicConf;

        if (topicRouteData.queueDatas != null) {
            this.queueDatas.addAll(topicRouteData.queueDatas);
        }

        if (topicRouteData.brokerDatas != null) {
            this.brokerDatas.addAll(topicRouteData.brokerDatas);
        }

        if (topicRouteData.filterServerTable != null) {
            this.filterServerTable.putAll(topicRouteData.filterServerTable);
        }

        if (topicRouteData.topicQueueMappingByBroker != null) {
            this.topicQueueMappingByBroker = new HashMap<String, TopicQueueMappingInfo>(topicRouteData.topicQueueMappingByBroker);
        }
    }

    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }

    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }

    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }

    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
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

    public Map<String, TopicQueueMappingInfo> getTopicQueueMappingByBroker() {
        return topicQueueMappingByBroker;
    }

    public void setTopicQueueMappingByBroker(Map<String, TopicQueueMappingInfo> topicQueueMappingByBroker) {
        this.topicQueueMappingByBroker = topicQueueMappingByBroker;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerDatas == null) ? 0 : brokerDatas.hashCode());
        result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.hashCode());
        result = prime * result + ((queueDatas == null) ? 0 : queueDatas.hashCode());
        result = prime * result + ((filterServerTable == null) ? 0 : filterServerTable.hashCode());
        result = prime * result + ((topicQueueMappingByBroker == null) ? 0 : topicQueueMappingByBroker.hashCode());
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
        if (brokerDatas == null) {
            if (other.brokerDatas != null)
                return false;
        } else if (!brokerDatas.equals(other.brokerDatas))
            return false;
        if (orderTopicConf == null) {
            if (other.orderTopicConf != null)
                return false;
        } else if (!orderTopicConf.equals(other.orderTopicConf))
            return false;
        if (queueDatas == null) {
            if (other.queueDatas != null)
                return false;
        } else if (!queueDatas.equals(other.queueDatas))
            return false;
        if (filterServerTable == null) {
            if (other.filterServerTable != null)
                return false;
        } else if (!filterServerTable.equals(other.filterServerTable))
            return false;
        if (topicQueueMappingByBroker == null) {
            if (other.topicQueueMappingByBroker != null)
                return false;
        } else if (!topicQueueMappingByBroker.equals(other.topicQueueMappingByBroker))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TopicRouteData [orderTopicConf=" + orderTopicConf + ", queueDatas=" + queueDatas
            + ", brokerDatas=" + brokerDatas + ", filterServerTable=" + filterServerTable + ", topicQueueMappingInfoTable=" + topicQueueMappingByBroker + "]";
    }
}
