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
package org.apache.rocketmq.tools.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;

public class MockObjectUtil {

    public static ClusterInfo createClusterInfo() {
        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, Set<String>> clusterAddrTable = new HashMap<>(3);
        Set<String> brokerNameSet = new HashSet<>(3);
        brokerNameSet.add("broker-a");
        clusterAddrTable.put("DefaultCluster", brokerNameSet);
        clusterInfo.setClusterAddrTable(clusterAddrTable);
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<>(3);
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("broker-a");
        HashMap<Long, String> brokerAddrs = new HashMap<>(2);
        brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerAddrTable.put("broker-a", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        return clusterInfo;
    }

    public static SubscriptionGroupWrapper createSubscriptionGroupWrapper() {
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable = new ConcurrentHashMap(2);
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName("group_test");
        subscriptionGroupTable.put("group_test", config);
        SubscriptionGroupConfig sysGroupConfig = new SubscriptionGroupConfig();
        sysGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP);
        subscriptionGroupTable.put(MixAll.TOOLS_CONSUMER_GROUP, sysGroupConfig);
        wrapper.setSubscriptionGroupTable(subscriptionGroupTable);
        wrapper.setDataVersion(new DataVersion());
        return wrapper;
    }

    public static TopicConfigSerializeWrapper createTopicConfigWrapper() {
        TopicConfigSerializeWrapper wrapper = new TopicConfigSerializeWrapper();
        TopicConfig config = new TopicConfig();
        config.setTopicName("topic_test");
        ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap(2);
        topicConfigTable.put("topic_test", config);
        wrapper.setTopicConfigTable(topicConfigTable);
        wrapper.setDataVersion(new DataVersion());
        return wrapper;
    }
}
