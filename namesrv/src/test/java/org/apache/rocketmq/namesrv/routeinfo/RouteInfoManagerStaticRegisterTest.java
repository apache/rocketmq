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
package org.apache.rocketmq.namesrv.routeinfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RouteInfoManagerStaticRegisterTest extends RouteInfoManagerTestBase {
    private static RouteInfoManager routeInfoManager;
    public static String clusterName = "cluster";
    public static String brokerPrefix = "broker";
    public static String topicPrefix = "topic";

    public static RouteInfoManagerTestBase.Cluster cluster;

    @Before
    public void setup() {
        routeInfoManager = new RouteInfoManager(new NamesrvConfig(), null);
        cluster = registerCluster(routeInfoManager,
            clusterName,
            brokerPrefix,
            3,
            3,
            topicPrefix,
            10);
    }

    @After
    public void terminate() {
        routeInfoManager.printAllPeriodically();

        for (BrokerData bd : cluster.brokerDataMap.values()) {
            unregisterBrokerAll(routeInfoManager, bd);
        }
    }

    @Test
    public void testGetAllClusterInfo() {
        ClusterInfo clusterInfo = routeInfoManager.getAllClusterInfo();
        Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();

        assertEquals(1, clusterAddrTable.size());
        assertEquals(cluster.getAllBrokerName(), clusterAddrTable.get(clusterName));
    }

    @Test
    public void testGetAllTopicList() {
        TopicList topicInfo = routeInfoManager.getAllTopicList();

        assertEquals(cluster.getAllTopicName(), topicInfo.getTopicList());
    }

    @Test
    public void testGetTopicsByCluster() {
        TopicList topicList = routeInfoManager.getTopicsByCluster(clusterName);
        assertEquals(cluster.getAllTopicName(), topicList.getTopicList());
    }

    @Test
    public void testPickupTopicRouteData() {
        String topic = getTopicName(topicPrefix, 0);

        TopicRouteData topicRouteData = routeInfoManager.pickupTopicRouteData(topic);

        TopicConfig topicConfig = cluster.topicConfig.get(topic);

        // check broker data
        Collections.sort(topicRouteData.getBrokerDatas());
        List<BrokerData> ans = new ArrayList<>(cluster.brokerDataMap.values());
        Collections.sort(ans);

        assertEquals(topicRouteData.getBrokerDatas(), ans);

        // check queue data
        HashSet<String> allBrokerNameInQueueData = new HashSet<>();

        for (QueueData queueData : topicRouteData.getQueueDatas()) {
            allBrokerNameInQueueData.add(queueData.getBrokerName());

            assertEquals(queueData.getWriteQueueNums(), topicConfig.getWriteQueueNums());
            assertEquals(queueData.getReadQueueNums(), topicConfig.getReadQueueNums());
            assertEquals(queueData.getPerm(), topicConfig.getPerm());
            assertEquals(queueData.getTopicSysFlag(), topicConfig.getTopicSysFlag());
        }

        assertEquals(allBrokerNameInQueueData, new HashSet<>(cluster.getAllBrokerName()));
    }

    @Test
    public void testDeleteTopic() {
        String topic = getTopicName(topicPrefix, 0);
        routeInfoManager.deleteTopic(topic);

        assertNull(routeInfoManager.pickupTopicRouteData(topic));
    }

    @Test
    public void testGetSystemTopicList() {
        TopicList topicList = routeInfoManager.getSystemTopicList();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetUnitTopics() {
        TopicList topicList = routeInfoManager.getUnitTopics();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetHasUnitSubTopicList() {
        TopicList topicList = routeInfoManager.getHasUnitSubTopicList();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetHasUnitSubUnUnitTopicList() {
        TopicList topicList = routeInfoManager.getHasUnitSubUnUnitTopicList();
        assertThat(topicList).isNotNull();
    }

}
