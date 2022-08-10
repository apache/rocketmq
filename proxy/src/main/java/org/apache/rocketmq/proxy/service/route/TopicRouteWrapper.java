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
package org.apache.rocketmq.proxy.service.route;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicRouteWrapper {

    private final TopicRouteData topicRouteData;
    private final String topicName;
    private final Map<String/* brokerName */, BrokerData> brokerNameRouteData = new HashMap<>();

    public TopicRouteWrapper(TopicRouteData topicRouteData, String topicName) {
        this.topicRouteData = topicRouteData;
        this.topicName = topicName;

        if (this.topicRouteData.getBrokerDatas() != null) {
            for (BrokerData brokerData : this.topicRouteData.getBrokerDatas()) {
                this.brokerNameRouteData.put(brokerData.getBrokerName(), brokerData);
            }
        }
    }

    public String getMasterAddr(String brokerName) {
        return this.brokerNameRouteData.get(brokerName).getBrokerAddrs().get(MixAll.MASTER_ID);
    }

    public String getMasterAddrPrefer(String brokerName) {
        HashMap<Long, String> brokerAddr = brokerNameRouteData.get(brokerName).getBrokerAddrs();
        String addr = brokerAddr.get(MixAll.MASTER_ID);
        if (addr == null) {
            Optional<Long> optional = brokerAddr.keySet().stream().findFirst();
            return optional.map(brokerAddr::get).orElse(null);
        }
        return addr;
    }

    public String getTopicName() {
        return topicName;
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public List<QueueData> getQueueDatas() {
        return this.topicRouteData.getQueueDatas();
    }

    public String getOrderTopicConf() {
        return this.topicRouteData.getOrderTopicConf();
    }
}
