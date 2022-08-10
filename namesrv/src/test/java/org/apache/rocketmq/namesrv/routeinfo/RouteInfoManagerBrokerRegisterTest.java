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

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RouteInfoManagerBrokerRegisterTest extends RouteInfoManagerTestBase {
    private static RouteInfoManager routeInfoManager;
    public static String clusterName = "cluster";
    public static String brokerPrefix = "broker";
    public static String topicPrefix = "topic";
    public static int brokerPerName = 3;
    public static int brokerNameNumber = 3;

    public static RouteInfoManagerTestBase.Cluster cluster;

    @Before
    public void setup() {
        routeInfoManager = new RouteInfoManager(new NamesrvConfig(), null);
        cluster = registerCluster(routeInfoManager,
            clusterName,
            brokerPrefix,
            brokerNameNumber,
            brokerPerName,
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

//    @Test
//    public void testScanNotActiveBroker() {
//        for (int j = 0; j < brokerNameNumber; j++) {
//            String brokerName = getBrokerName(brokerPrefix, j);
//
//            for (int i = 0; i < brokerPerName; i++) {
//                String brokerAddr = getBrokerAddr(clusterName, brokerName, i);
//
//                // set not active
//                routeInfoManager.updateBrokerInfoUpdateTimestamp(brokerAddr, 0);
//
//                assertEquals(1, routeInfoManager.scanNotActiveBroker());
//            }
//        }
//
//    }

    @Test
    public void testMasterChangeFromSlave() {
        String topicName = getTopicName(topicPrefix, 0);
        String brokerName = getBrokerName(brokerPrefix, 0);

        String originMasterAddr = getBrokerAddr(clusterName, brokerName, MixAll.MASTER_ID);
        TopicRouteData topicRouteData = routeInfoManager.pickupTopicRouteData(topicName);
        BrokerData brokerDataOrigin = findBrokerDataByBrokerName(topicRouteData.getBrokerDatas(), brokerName);

        // check origin master address
        Assert.assertEquals(brokerDataOrigin.getBrokerAddrs().get(MixAll.MASTER_ID), originMasterAddr);

        // master changed
        String newMasterAddr = getBrokerAddr(clusterName, brokerName, 1);
        registerBrokerWithTopicConfig(routeInfoManager,
            clusterName,
            newMasterAddr,
            brokerName,
            MixAll.MASTER_ID,
            newMasterAddr,
            cluster.topicConfig,
            new ArrayList<>());

        topicRouteData = routeInfoManager.pickupTopicRouteData(topicName);
        brokerDataOrigin = findBrokerDataByBrokerName(topicRouteData.getBrokerDatas(), brokerName);

        // check new master address
        assertEquals(brokerDataOrigin.getBrokerAddrs().get(MixAll.MASTER_ID), newMasterAddr);
    }

    @Test
    public void testUnregisterBroker() {
        String topicName = getTopicName(topicPrefix, 0);
        String brokerName = getBrokerName(brokerPrefix, 0);
        long unregisterBrokerId = 2;

        unregisterBroker(routeInfoManager, cluster.brokerDataMap.get(brokerName), unregisterBrokerId);

        TopicRouteData topicRouteData = routeInfoManager.pickupTopicRouteData(topicName);
        HashMap<Long, String> brokerAddrs = findBrokerDataByBrokerName(topicRouteData.getBrokerDatas(), brokerName).getBrokerAddrs();

        assertFalse(brokerAddrs.containsKey(unregisterBrokerId));
    }
}
