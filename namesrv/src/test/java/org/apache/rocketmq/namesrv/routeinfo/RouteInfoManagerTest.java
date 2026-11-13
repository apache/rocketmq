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

import io.netty.channel.Channel;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class RouteInfoManagerTest {

    private static RouteInfoManager routeInfoManager;

    @Before
    public void setup() {
        routeInfoManager = new RouteInfoManager(new NamesrvConfig(), null);
        routeInfoManager.start();
        testRegisterBroker();
    }

    @After
    public void terminate() {
        routeInfoManager.shutdown();
        routeInfoManager.printAllPeriodically();
        routeInfoManager.unregisterBroker("default-cluster", "127.0.0.1:10911", "default-broker", 1234);
    }

    @Test
    public void testGetAllClusterInfo() {
        byte[] clusterInfo = routeInfoManager.getAllClusterInfo().encode();
        assertThat(clusterInfo).isNotNull();
    }

    @Test
    public void testQueryBrokerTopicConfig() {
        {
            DataVersion targetVersion = new DataVersion();
            targetVersion.setCounter(new AtomicLong(10L));
            targetVersion.setTimestamp(100L);

            DataVersion dataVersion = routeInfoManager.queryBrokerTopicConfig("default-cluster", "127.0.0.1:10911");
            assertThat(dataVersion.equals(targetVersion)).isTrue();
        }

        {
            // register broker default-cluster-1 with the same addr, then test
            DataVersion targetVersion = new DataVersion();
            targetVersion.setCounter(new AtomicLong(20L));
            targetVersion.setTimestamp(200L);

            ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
            topicConfigConcurrentHashMap.put("unit-test-0", new TopicConfig("unit-test-0"));
            topicConfigConcurrentHashMap.put("unit-test-1", new TopicConfig("unit-test-1"));

            TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
            topicConfigSerializeWrapper.setDataVersion(targetVersion);
            topicConfigSerializeWrapper.setTopicConfigTable(topicConfigConcurrentHashMap);
            Channel channel = mock(Channel.class);
            RegisterBrokerResult registerBrokerResult = routeInfoManager.registerBroker("default-cluster-1", "127.0.0.1:10911", "default-broker-1", 1234, "127.0.0.1:1001", "",
                    null, topicConfigSerializeWrapper, new ArrayList<>(), channel);
            assertThat(registerBrokerResult).isNotNull();

            DataVersion dataVersion0 = routeInfoManager.queryBrokerTopicConfig("default-cluster", "127.0.0.1:10911");
            assertThat(targetVersion.equals(dataVersion0)).isFalse();

            DataVersion dataVersion1 = routeInfoManager.queryBrokerTopicConfig("default-cluster-1", "127.0.0.1:10911");
            assertThat(targetVersion.equals(dataVersion1)).isTrue();
        }

        // unregister broker default-cluster-1, then test
        {
            routeInfoManager.unregisterBroker("default-cluster-1", "127.0.0.1:10911", "default-broker-1", 1234);
            assertThat(null != routeInfoManager.queryBrokerTopicConfig("default-cluster", "127.0.0.1:10911")).isTrue();
            assertThat(null == routeInfoManager.queryBrokerTopicConfig("default-cluster-1", "127.0.0.1:10911")).isTrue();
        }
    }

    @Test
    public void testGetAllTopicList() {
        byte[] topicInfo = routeInfoManager.getAllTopicList().encode();
        Assert.assertTrue(topicInfo != null);
        assertThat(topicInfo).isNotNull();
    }

    @Test
    public void testRegisterBroker() {
        DataVersion dataVersion = new DataVersion();
        dataVersion.setCounter(new AtomicLong(10L));
        dataVersion.setTimestamp(100L);

        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
        topicConfigConcurrentHashMap.put("unit-test0", new TopicConfig("unit-test0"));
        topicConfigConcurrentHashMap.put("unit-test1", new TopicConfig("unit-test1"));

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(dataVersion);
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigConcurrentHashMap);
        Channel channel = mock(Channel.class);
        RegisterBrokerResult registerBrokerResult = routeInfoManager.registerBroker("default-cluster", "127.0.0.1:10911", "default-broker", 1234, "127.0.0.1:1001", "",
                null, topicConfigSerializeWrapper, new ArrayList<>(), channel);
        assertThat(registerBrokerResult).isNotNull();
    }

    @Test
    public void testUnregisterBrokerWithStaleQueueData() throws Exception {
        // Two acting-master groups serving the same topics. healthy-broker
        // keeps a slave after its master unregisters, while stale-broker's
        // entry in brokerAddrTable is already gone and only its queueData is
        // left behind - the state an earlier aborted unregister batch leaves.
        ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        topicConfigTable.put("topic-a", new TopicConfig("topic-a"));
        topicConfigTable.put("topic-b", new TopicConfig("topic-b"));
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(new DataVersion());
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);
        Channel channel = mock(Channel.class);

        assertThat(routeInfoManager.registerBroker("default-cluster", "127.0.0.1:40911", "stale-broker", 0, "127.0.0.1:1002", "",
            null, true, topicConfigSerializeWrapper, new ArrayList<>(), channel)).isNotNull();
        assertThat(routeInfoManager.registerBroker("default-cluster", "127.0.0.1:50911", "healthy-broker", 0, "127.0.0.1:1003", "",
            null, true, topicConfigSerializeWrapper, new ArrayList<>(), channel)).isNotNull();
        // the surviving slave keeps healthy-broker in the reducedBroker path
        assertThat(routeInfoManager.registerBroker("default-cluster", "127.0.0.1:50912", "healthy-broker", 1, "127.0.0.1:1004", "",
            null, true, topicConfigSerializeWrapper, new ArrayList<>(), channel)).isNotNull();

        Field brokerAddrTableField = RouteInfoManager.class.getDeclaredField("brokerAddrTable");
        brokerAddrTableField.setAccessible(true);
        Map<String, BrokerData> brokerAddrTable = (Map<String, BrokerData>) brokerAddrTableField.get(routeInfoManager);
        brokerAddrTable.remove("stale-broker");

        UnRegisterBrokerRequestHeader staleRequest = new UnRegisterBrokerRequestHeader();
        staleRequest.setClusterName("default-cluster");
        staleRequest.setBrokerAddr("127.0.0.1:40911");
        staleRequest.setBrokerName("stale-broker");
        staleRequest.setBrokerId(0L);
        UnRegisterBrokerRequestHeader healthyRequest = new UnRegisterBrokerRequestHeader();
        healthyRequest.setClusterName("default-cluster");
        healthyRequest.setBrokerAddr("127.0.0.1:50911");
        healthyRequest.setBrokerName("healthy-broker");
        healthyRequest.setBrokerId(0L);

        Set<UnRegisterBrokerRequestHeader> requests = new LinkedHashSet<>();
        requests.add(staleRequest);
        requests.add(healthyRequest);
        routeInfoManager.unRegisterBroker(requests);

        // healthy-broker's master is offline, so its queueData must be wiped
        // to read-only; before the fix the stale entry NPE'd first and the
        // cleanup of the whole batch was skipped.
        Field topicQueueTableField = RouteInfoManager.class.getDeclaredField("topicQueueTable");
        topicQueueTableField.setAccessible(true);
        Map<String, Map<String, QueueData>> topicQueueTable = (Map<String, Map<String, QueueData>>) topicQueueTableField.get(routeInfoManager);
        assertThat(topicQueueTable.get("topic-a").get("healthy-broker").getPerm()).isEqualTo(PermName.PERM_READ);
        assertThat(topicQueueTable.get("topic-b").get("healthy-broker").getPerm()).isEqualTo(PermName.PERM_READ);
    }

    @Test
    public void testWipeWritePermOfBrokerByLock() throws Exception {
        Map<String, QueueData> qdMap = new HashMap<>();

        QueueData qd = new QueueData();
        qd.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        qd.setBrokerName("broker-a");
        qdMap.put("broker-a",qd);
        HashMap<String, Map<String, QueueData>> topicQueueTable = new HashMap<>();
        topicQueueTable.put("topic-a", qdMap);

        Field filed = RouteInfoManager.class.getDeclaredField("topicQueueTable");
        filed.setAccessible(true);
        filed.set(routeInfoManager, topicQueueTable);

        int addTopicCnt = routeInfoManager.wipeWritePermOfBrokerByLock("broker-a");
        assertThat(addTopicCnt).isEqualTo(1);
        assertThat(qd.getPerm()).isEqualTo(PermName.PERM_READ);

    }

    @Test
    public void testPickupTopicRouteData() {
        TopicRouteData result = routeInfoManager.pickupTopicRouteData("unit_test");
        assertThat(result).isNull();
    }

    @Test
    public void testGetSystemTopicList() {
        byte[] topicList = routeInfoManager.getSystemTopicList().encode();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetTopicsByCluster() {
        byte[] topicList = routeInfoManager.getTopicsByCluster("default-cluster").encode();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetUnitTopics() {
        byte[] topicList = routeInfoManager.getUnitTopics().encode();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetHasUnitSubTopicList() {
        byte[] topicList = routeInfoManager.getHasUnitSubTopicList().encode();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testGetHasUnitSubUnUnitTopicList() {
        byte[] topicList = routeInfoManager.getHasUnitSubUnUnitTopicList().encode();
        assertThat(topicList).isNotNull();
    }

    @Test
    public void testAddWritePermOfBrokerByLock() throws Exception {
        Map<String, QueueData> qdMap = new HashMap<>();
        QueueData qd = new QueueData();
        qd.setPerm(PermName.PERM_READ);
        qd.setBrokerName("broker-a");
        qdMap.put("broker-a",qd);
        HashMap<String, Map<String, QueueData>> topicQueueTable = new HashMap<>();
        topicQueueTable.put("topic-a", qdMap);

        Field filed = RouteInfoManager.class.getDeclaredField("topicQueueTable");
        filed.setAccessible(true);
        filed.set(routeInfoManager, topicQueueTable);

        int addTopicCnt = routeInfoManager.addWritePermOfBrokerByLock("broker-a");
        assertThat(addTopicCnt).isEqualTo(1);
        assertThat(qd.getPerm()).isEqualTo(PermName.PERM_READ | PermName.PERM_WRITE);

    }
}
