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

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Spy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class RouteInfoManagerNewTest {
    private RouteInfoManager routeInfoManager;
    private static final String DEFAULT_CLUSTER = "Default_Cluster";
    private static final String DEFAULT_BROKER = "Default_Broker";
    private static final String DEFAULT_ADDR_PREFIX = "127.0.0.1:";
    private static final String DEFAULT_ADDR = DEFAULT_ADDR_PREFIX + "10911";

    // Synced from RouteInfoManager
    private static final int BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

    @Spy
    private static NamesrvConfig config = spy(new NamesrvConfig());

    @Before
    public void setup() {
        config.setSupportActingMaster(true);
        routeInfoManager = new RouteInfoManager(config, null);
        routeInfoManager.start();
    }

    @After
    public void tearDown() throws Exception {
        routeInfoManager.shutdown();
    }

    @Test
    public void getAllClusterInfo() {
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker()
            .cluster("AnotherCluster")
            .name("AnotherBroker")
            .addr(DEFAULT_ADDR_PREFIX + 30911), "TestTopic1");

        final byte[] content = routeInfoManager.getAllClusterInfo().encode();

        final ClusterInfo clusterInfo = ClusterInfo.decode(content, ClusterInfo.class);

        assertThat(clusterInfo.retrieveAllClusterNames()).contains(DEFAULT_CLUSTER, "AnotherCluster");
        assertThat(clusterInfo.getBrokerAddrTable().keySet()).contains(DEFAULT_BROKER, "AnotherBroker");

        final List<String> addrList = Arrays.asList(clusterInfo.getBrokerAddrTable().get(DEFAULT_BROKER).getBrokerAddrs().get(0L),
            clusterInfo.getBrokerAddrTable().get("AnotherBroker").getBrokerAddrs().get(0L));
        assertThat(addrList).contains(DEFAULT_ADDR, DEFAULT_ADDR_PREFIX + 30911);
    }

    @Test
    public void deleteTopic() {
        String testTopic = "TestTopic";
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), testTopic);

        routeInfoManager.deleteTopic(testTopic);

        assertThat(routeInfoManager.pickupTopicRouteData(testTopic)).isNull();

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), testTopic);
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker().cluster("AnotherCluster").name("AnotherBroker"),
            testTopic);

        assertThat(routeInfoManager.pickupTopicRouteData(testTopic).getBrokerDatas().size()).isEqualTo(2);
        routeInfoManager.deleteTopic(testTopic, DEFAULT_CLUSTER);

        assertThat(routeInfoManager.pickupTopicRouteData(testTopic).getBrokerDatas().size()).isEqualTo(1);
        assertThat(routeInfoManager.pickupTopicRouteData(testTopic).getBrokerDatas().get(0).getBrokerName()).isEqualTo("AnotherBroker");
    }

    @Test
    public void getAllTopicList() {
        byte[] content = routeInfoManager.getAllTopicList().encode();

        TopicList topicList = TopicList.decode(content, TopicList.class);
        assertThat(topicList.getTopicList()).isEmpty();

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic", "TestTopic1", "TestTopic2");

        content = routeInfoManager.getAllTopicList().encode();

        topicList = TopicList.decode(content, TopicList.class);
        assertThat(topicList.getTopicList()).contains("TestTopic", "TestTopic1", "TestTopic2");
    }

    @Test
    public void registerBroker() {
        // Register master broker
        final RegisterBrokerResult masterResult = registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");

        assertThat(masterResult).isNotNull();
        assertThat(masterResult.getHaServerAddr()).isNull();
        assertThat(masterResult.getMasterAddr()).isNull();

        // Register slave broker

        final BrokerBasicInfo slaveBroker = BrokerBasicInfo.defaultBroker()
            .id(1).addr(DEFAULT_ADDR_PREFIX + 30911).haAddr(DEFAULT_ADDR_PREFIX + 40911);

        final RegisterBrokerResult slaveResult = registerBrokerWithNormalTopic(slaveBroker, "TestTopic");

        assertThat(slaveResult).isNotNull();
        assertThat(slaveResult.getHaServerAddr()).isEqualTo(DEFAULT_ADDR_PREFIX + 20911);
        assertThat(slaveResult.getMasterAddr()).isEqualTo(DEFAULT_ADDR);
    }

    @Test
    public void unregisterBroker() {
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic", "TestTopic1", "TestTopic2");
        routeInfoManager.unregisterBroker(DEFAULT_CLUSTER, DEFAULT_ADDR, DEFAULT_BROKER, 0);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic2")).isNull();

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic", "TestTopic1", "TestTopic2");

        routeInfoManager.submitUnRegisterBrokerRequest(BrokerBasicInfo.defaultBroker().unRegisterRequest());
        await().atMost(Duration.ofSeconds(5)).until(() -> routeInfoManager.blockedUnRegisterRequests() == 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic2")).isNull();
    }

    @Test
    public void registerSlaveBroker() {
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsKeys(0L);

        registerBrokerWithNormalTopic(BrokerBasicInfo.slaveBroker(), "TestTopic");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsKeys(0L, 1L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(BrokerBasicInfo.defaultBroker().brokerAddr, BrokerBasicInfo.slaveBroker().brokerAddr);
    }

    @Test
    public void createNewTopic() {
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");
        registerBrokerWithNormalTopic(BrokerBasicInfo.slaveBroker(), "TestTopic");

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic", "TestTopic1");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1").getBrokerDatas().get(0).getBrokerAddrs()).containsKeys(0L, 1L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(BrokerBasicInfo.defaultBroker().brokerAddr, BrokerBasicInfo.slaveBroker().brokerAddr);
    }

    @Test
    public void switchBrokerRole() {
        final BrokerBasicInfo masterBroker = BrokerBasicInfo.defaultBroker().enableActingMaster(false);
        final BrokerBasicInfo slaveBroker = BrokerBasicInfo.slaveBroker().enableActingMaster(false);
        registerBrokerWithNormalTopic(masterBroker, "TestTopic");
        registerBrokerWithNormalTopic(slaveBroker, "TestTopic");

        // Master Down
        routeInfoManager.unregisterBroker(masterBroker.clusterName, masterBroker.brokerAddr, masterBroker.brokerName, 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsOnlyKeys(1L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(slaveBroker.brokerAddr);

        // Switch slave to master
        slaveBroker.id(0).dataVersion.nextVersion();
        registerBrokerWithNormalTopic(slaveBroker, "TestTopic");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsOnlyKeys(0L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(slaveBroker.brokerAddr);

        // Old master switch to slave
        masterBroker.id(1).dataVersion.nextVersion();
        registerBrokerWithNormalTopic(masterBroker, "TestTopic");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsKeys(0L, 1L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(BrokerBasicInfo.defaultBroker().brokerAddr, BrokerBasicInfo.slaveBroker().brokerAddr);
    }

    @Test
    public void unRegisterSlaveBroker() {
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");
        final BrokerBasicInfo slaveBroker = BrokerBasicInfo.slaveBroker();
        registerBrokerWithNormalTopic(slaveBroker, "TestTopic");

        routeInfoManager.unregisterBroker(slaveBroker.clusterName, slaveBroker.brokerAddr, slaveBroker.brokerName, 1);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().size()).isEqualTo(1);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0)
            .getBrokerAddrs().get(0L)).isEqualTo(BrokerBasicInfo.defaultBroker().brokerAddr);

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");
        registerBrokerWithNormalTopic(slaveBroker, "TestTopic");

        routeInfoManager.submitUnRegisterBrokerRequest(slaveBroker.unRegisterRequest());
        await().atMost(Duration.ofSeconds(5)).until(() -> routeInfoManager.blockedUnRegisterRequests() == 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().size()).isEqualTo(1);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0)
            .getBrokerAddrs().get(0L)).isEqualTo(BrokerBasicInfo.defaultBroker().brokerAddr);
    }

    @Test
    public void unRegisterMasterBroker() {
        final BrokerBasicInfo masterBroker = BrokerBasicInfo.defaultBroker();
        masterBroker.enableActingMaster = true;
        registerBrokerWithNormalTopic(masterBroker, "TestTopic");
        final BrokerBasicInfo slaveBroker = BrokerBasicInfo.slaveBroker();
        registerBrokerWithNormalTopic(slaveBroker, "TestTopic");

        routeInfoManager.unregisterBroker(masterBroker.clusterName, masterBroker.brokerAddr, masterBroker.brokerName, 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().size()).isEqualTo(1);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0)
            .getBrokerAddrs().get(0L)).isEqualTo(slaveBroker.brokerAddr);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getQueueDatas().size()).isEqualTo(1);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getQueueDatas().get(0).getPerm()).isEqualTo(PermName.PERM_READ);
    }

    @Test
    public void unRegisterMasterBrokerOldVersion() {
        final BrokerBasicInfo masterBroker = BrokerBasicInfo.defaultBroker();
        masterBroker.enableActingMaster = false;
        registerBrokerWithNormalTopic(masterBroker, "TestTopic");
        final BrokerBasicInfo slaveBroker = BrokerBasicInfo.slaveBroker();
        slaveBroker.enableActingMaster = false;
        registerBrokerWithNormalTopic(slaveBroker, "TestTopic");

        routeInfoManager.unregisterBroker(masterBroker.clusterName, masterBroker.brokerAddr, masterBroker.brokerName, 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().size()).isEqualTo(1);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0)
            .getBrokerAddrs().get(0L)).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getQueueDatas().size()).isEqualTo(1);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getQueueDatas().get(0).getPerm()).isEqualTo(PermName.PERM_READ | PermName.PERM_WRITE);
    }

    @Test
    public void submitMultiUnRegisterRequests() {
        final BrokerBasicInfo master1 = BrokerBasicInfo.defaultBroker();
        final BrokerBasicInfo master2 = BrokerBasicInfo.defaultBroker().name(DEFAULT_BROKER + 1).addr(DEFAULT_ADDR + 9);
        registerBrokerWithNormalTopic(master1, "TestTopic1");
        registerBrokerWithNormalTopic(master2, "TestTopic2");

        routeInfoManager.submitUnRegisterBrokerRequest(master1.unRegisterRequest());
        routeInfoManager.submitUnRegisterBrokerRequest(master2.unRegisterRequest());

        await().atMost(Duration.ofSeconds(5)).until(() -> routeInfoManager.blockedUnRegisterRequests() == 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic2")).isNull();
    }

    @Test
    public void isBrokerTopicConfigChanged() {
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");

        final DataVersion dataVersion = routeInfoManager.queryBrokerTopicConfig(DEFAULT_CLUSTER, DEFAULT_ADDR);

        assertThat(routeInfoManager.isBrokerTopicConfigChanged(DEFAULT_CLUSTER, DEFAULT_ADDR, dataVersion)).isFalse();

        DataVersion newVersion = new DataVersion();
        newVersion.setTimestamp(System.currentTimeMillis() + 1000);
        newVersion.setCounter(new AtomicLong(dataVersion.getCounter().get()));

        assertThat(routeInfoManager.isBrokerTopicConfigChanged(DEFAULT_CLUSTER, DEFAULT_ADDR, newVersion)).isTrue();

        newVersion = new DataVersion();
        newVersion.setTimestamp(dataVersion.getTimestamp());
        newVersion.setCounter(new AtomicLong(dataVersion.getCounter().get() + 1));

        assertThat(routeInfoManager.isBrokerTopicConfigChanged(DEFAULT_CLUSTER, DEFAULT_ADDR, newVersion)).isTrue();
    }

    @Test
    public void isTopicConfigChanged() {
        final BrokerBasicInfo brokerInfo = BrokerBasicInfo.defaultBroker();
        assertThat(routeInfoManager.isTopicConfigChanged(DEFAULT_CLUSTER,
            DEFAULT_ADDR,
            brokerInfo.dataVersion,
            DEFAULT_BROKER, "TestTopic")).isTrue();

        registerBrokerWithNormalTopic(brokerInfo, "TestTopic");

        assertThat(routeInfoManager.isTopicConfigChanged(DEFAULT_CLUSTER,
            DEFAULT_ADDR,
            brokerInfo.dataVersion,
            DEFAULT_BROKER, "TestTopic")).isFalse();

        assertThat(routeInfoManager.isTopicConfigChanged(DEFAULT_CLUSTER,
            DEFAULT_ADDR,
            brokerInfo.dataVersion,
            DEFAULT_BROKER, "TestTopic1")).isTrue();
    }

    @Test
    public void queryBrokerTopicConfig() {
        final BrokerBasicInfo basicInfo = BrokerBasicInfo.defaultBroker();
        registerBrokerWithNormalTopic(basicInfo, "TestTopic");

        final DataVersion dataVersion = routeInfoManager.queryBrokerTopicConfig(DEFAULT_CLUSTER, DEFAULT_ADDR);

        assertThat(basicInfo.dataVersion.equals(dataVersion)).isTrue();
    }

    @Test
    public void wipeWritePermOfBrokerByLock() {
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getQueueDatas().get(0).getPerm()).isEqualTo(6);

        routeInfoManager.wipeWritePermOfBrokerByLock(DEFAULT_BROKER);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getQueueDatas().get(0).getPerm()).isEqualTo(4);
    }

    @Test
    public void pickupTopicRouteData() {
        String testTopic = "TestTopic";
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), testTopic);

        TopicRouteData data = routeInfoManager.pickupTopicRouteData(testTopic);
        assertThat(data.getBrokerDatas().size()).isEqualTo(1);
        assertThat(data.getBrokerDatas().get(0).getBrokerName()).isEqualTo(DEFAULT_BROKER);
        assertThat(data.getBrokerDatas().get(0).getBrokerAddrs().get(0L)).isEqualTo(DEFAULT_ADDR);
        assertThat(data.getQueueDatas().size()).isEqualTo(1);
        assertThat(data.getQueueDatas().get(0).getBrokerName()).isEqualTo(DEFAULT_BROKER);
        assertThat(data.getQueueDatas().get(0).getReadQueueNums()).isEqualTo(8);
        assertThat(data.getQueueDatas().get(0).getWriteQueueNums()).isEqualTo(8);
        assertThat(data.getQueueDatas().get(0).getPerm()).isEqualTo(6);

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker().name("AnotherBroker"), testTopic);
        data = routeInfoManager.pickupTopicRouteData(testTopic);

        assertThat(data.getBrokerDatas().size()).isEqualTo(2);
        assertThat(data.getQueueDatas().size()).isEqualTo(2);

        List<String> brokerList =
            Arrays.asList(data.getBrokerDatas().get(0).getBrokerName(), data.getBrokerDatas().get(1).getBrokerName());
        assertThat(brokerList).contains(DEFAULT_BROKER, "AnotherBroker");

        brokerList =
            Arrays.asList(data.getQueueDatas().get(0).getBrokerName(), data.getQueueDatas().get(1).getBrokerName());
        assertThat(brokerList).contains(DEFAULT_BROKER, "AnotherBroker");
    }

    @Test
    public void pickupTopicRouteDataWithSlave() {
        String testTopic = "TestTopic";
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), testTopic);
        registerBrokerWithNormalTopic(BrokerBasicInfo.slaveBroker(), testTopic);

        TopicRouteData routeData = routeInfoManager.pickupTopicRouteData(testTopic);

        assertThat(routeData.getBrokerDatas().get(0).getBrokerAddrs()).hasSize(2);
        assertThat(PermName.isWriteable(routeData.getQueueDatas().get(0).getPerm())).isTrue();

        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.defaultBroker().unRegisterRequest()));
        routeData = routeInfoManager.pickupTopicRouteData(testTopic);

        assertThat(routeData.getBrokerDatas().get(0).getBrokerAddrs()).hasSize(1);
        assertThat(PermName.isWriteable(routeData.getQueueDatas().get(0).getPerm())).isFalse();

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), testTopic);
        routeData = routeInfoManager.pickupTopicRouteData(testTopic);

        assertThat(routeData.getBrokerDatas().get(0).getBrokerAddrs()).hasSize(2);
        assertThat(PermName.isWriteable(routeData.getQueueDatas().get(0).getPerm())).isTrue();
    }

    @Test
    public void scanNotActiveBroker() {
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic");
        routeInfoManager.scanNotActiveBroker();
    }

    @Test
    public void pickupPartitionOrderTopicRouteData() {
        String orderTopic = "PartitionOrderTopicTest";

        // Case 1: Register global order topic with slave
        registerBrokerWithOrderTopic(BrokerBasicInfo.slaveBroker(), orderTopic);

        TopicRouteData orderRoute = routeInfoManager.pickupTopicRouteData(orderTopic);

        // Acting master check
        assertThat(orderRoute.getBrokerDatas().get(0).getBrokerAddrs())
            .containsOnlyKeys(MixAll.MASTER_ID);
        assertThat(orderRoute.getBrokerDatas().get(0).getBrokerAddrs())
            .containsValue(BrokerBasicInfo.slaveBroker().brokerAddr);
        assertThat(PermName.isWriteable(orderRoute.getQueueDatas().get(0).getPerm())).isFalse();

        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.slaveBroker().unRegisterRequest()));

        // Case 2: Register global order topic with master and slave, then unregister master

        registerBrokerWithOrderTopic(BrokerBasicInfo.slaveBroker(), orderTopic);
        registerBrokerWithOrderTopic(BrokerBasicInfo.defaultBroker(), orderTopic);
        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.defaultBroker().unRegisterRequest()));

        orderRoute = routeInfoManager.pickupTopicRouteData(orderTopic);

        // Acting master check
        assertThat(orderRoute.getBrokerDatas().get(0).getBrokerAddrs())
            .containsOnlyKeys(MixAll.MASTER_ID);
        assertThat(orderRoute.getBrokerDatas().get(0).getBrokerAddrs())
            .containsValue(BrokerBasicInfo.slaveBroker().brokerAddr);
        assertThat(PermName.isWriteable(orderRoute.getQueueDatas().get(0).getPerm())).isFalse();

        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.slaveBroker().unRegisterRequest()));

        // Case 3: Register two broker groups, only one group enable acting master
        registerBrokerWithOrderTopic(BrokerBasicInfo.slaveBroker(), orderTopic);
        registerBrokerWithOrderTopic(BrokerBasicInfo.defaultBroker(), orderTopic);

        final BrokerBasicInfo master1 = BrokerBasicInfo.defaultBroker().name(DEFAULT_BROKER + "_ANOTHER");
        final BrokerBasicInfo slave1 = BrokerBasicInfo.slaveBroker().name(DEFAULT_BROKER + "_ANOTHER");

        registerBrokerWithOrderTopic(master1, orderTopic);
        registerBrokerWithOrderTopic(slave1, orderTopic);

        orderRoute = routeInfoManager.pickupTopicRouteData(orderTopic);

        assertThat(orderRoute.getBrokerDatas()).hasSize(2);
        assertThat(orderRoute.getQueueDatas()).hasSize(2);

        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.defaultBroker().unRegisterRequest()));
        orderRoute = routeInfoManager.pickupTopicRouteData(orderTopic);

        assertThat(orderRoute.getBrokerDatas()).hasSize(2);
        assertThat(orderRoute.getQueueDatas()).hasSize(2);

        for (final BrokerData brokerData : orderRoute.getBrokerDatas()) {
            if (brokerData.getBrokerAddrs().size() == 1) {
                assertThat(brokerData.getBrokerAddrs()).containsOnlyKeys(MixAll.MASTER_ID);
                assertThat(brokerData.getBrokerAddrs()).containsValue(BrokerBasicInfo.slaveBroker().brokerAddr);
            } else if (brokerData.getBrokerAddrs().size() == 2) {
                assertThat(brokerData.getBrokerAddrs()).containsKeys(MixAll.MASTER_ID, (long) slave1.brokerId);
                assertThat(brokerData.getBrokerAddrs()).containsValues(master1.brokerAddr, slave1.brokerAddr);
            } else {
                throw new RuntimeException("Shouldn't reach here");
            }
        }
    }

    @Test
    public void pickupGlobalOrderTopicRouteData() {
        String orderTopic = "GlobalOrderTopicTest";

        // Case 1: Register global order topic with slave
        registerBrokerWithGlobalOrderTopic(BrokerBasicInfo.slaveBroker(), orderTopic);

        TopicRouteData orderRoute = routeInfoManager.pickupTopicRouteData(orderTopic);

        // Acting master check
        assertThat(orderRoute.getBrokerDatas().get(0).getBrokerAddrs())
            .containsOnlyKeys(MixAll.MASTER_ID);
        assertThat(orderRoute.getBrokerDatas().get(0).getBrokerAddrs())
            .containsValue(BrokerBasicInfo.slaveBroker().brokerAddr);
        assertThat(PermName.isWriteable(orderRoute.getQueueDatas().get(0).getPerm())).isFalse();

        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.slaveBroker().unRegisterRequest()));

        // Case 2: Register global order topic with master and slave, then unregister master

        registerBrokerWithGlobalOrderTopic(BrokerBasicInfo.slaveBroker(), orderTopic);
        registerBrokerWithGlobalOrderTopic(BrokerBasicInfo.defaultBroker(), orderTopic);
        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.defaultBroker().unRegisterRequest()));

        // Acting master check
        assertThat(orderRoute.getBrokerDatas().get(0).getBrokerAddrs())
            .containsOnlyKeys(MixAll.MASTER_ID);
        assertThat(orderRoute.getBrokerDatas().get(0).getBrokerAddrs())
            .containsValue(BrokerBasicInfo.slaveBroker().brokerAddr);
        assertThat(PermName.isWriteable(orderRoute.getQueueDatas().get(0).getPerm())).isFalse();
    }

    @Test
    public void registerOnlySlaveBroker() {
        final String testTopic = "TestTopic";

        // Case 1: Only slave broker
        registerBrokerWithNormalTopic(BrokerBasicInfo.slaveBroker(), testTopic);
        assertThat(routeInfoManager.pickupTopicRouteData(testTopic)).isNotNull();
        int topicPerm = routeInfoManager.pickupTopicRouteData(testTopic).getQueueDatas().get(0).getPerm();
        assertThat(PermName.isWriteable(topicPerm)).isFalse();
        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.slaveBroker().unRegisterRequest()));

        // Case 2: Register master, and slave, then unregister master, finally recover master
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), testTopic);
        registerBrokerWithNormalTopic(BrokerBasicInfo.slaveBroker(), testTopic);

        assertThat(routeInfoManager.pickupTopicRouteData(testTopic)).isNotNull();
        topicPerm = routeInfoManager.pickupTopicRouteData(testTopic).getQueueDatas().get(0).getPerm();
        assertThat(PermName.isWriteable(topicPerm)).isTrue();

        routeInfoManager.unRegisterBroker(Sets.newHashSet(BrokerBasicInfo.defaultBroker().unRegisterRequest()));
        assertThat(routeInfoManager.pickupTopicRouteData(testTopic)).isNotNull();
        topicPerm = routeInfoManager.pickupTopicRouteData(testTopic).getQueueDatas().get(0).getPerm();
        assertThat(PermName.isWriteable(topicPerm)).isFalse();

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), testTopic);
        assertThat(routeInfoManager.pickupTopicRouteData(testTopic)).isNotNull();
        topicPerm = routeInfoManager.pickupTopicRouteData(testTopic).getQueueDatas().get(0).getPerm();
        assertThat(PermName.isWriteable(topicPerm)).isTrue();
    }

    @Test
    public void onChannelDestroy() {
        Channel channel = mock(Channel.class);

        registerBroker(BrokerBasicInfo.defaultBroker(), channel, null, "TestTopic", "TestTopic1");
        routeInfoManager.onChannelDestroy(channel);
        await().atMost(Duration.ofSeconds(5)).until(() -> routeInfoManager.blockedUnRegisterRequests() == 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNull();

        final BrokerBasicInfo masterBroker = BrokerBasicInfo.defaultBroker().enableActingMaster(false);
        final BrokerBasicInfo slaveBroker = BrokerBasicInfo.slaveBroker().enableActingMaster(false);

        Channel masterChannel = mock(Channel.class);
        Channel slaveChannel = mock(Channel.class);

        registerBroker(masterBroker, masterChannel, null, "TestTopic");
        registerBroker(slaveBroker, slaveChannel, null, "TestTopic");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsKeys(0L, 1L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(masterBroker.brokerAddr, slaveBroker.brokerAddr);

        routeInfoManager.onChannelDestroy(masterChannel);
        await().atMost(Duration.ofSeconds(5)).until(() -> routeInfoManager.blockedUnRegisterRequests() == 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsOnlyKeys(1L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(slaveBroker.brokerAddr);

        routeInfoManager.onChannelDestroy(slaveChannel);
        await().atMost(Duration.ofSeconds(5)).until(() -> routeInfoManager.blockedUnRegisterRequests() == 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNull();
    }

    @Test
    public void switchBrokerRole_ChannelDestroy() {
        final BrokerBasicInfo masterBroker = BrokerBasicInfo.defaultBroker().enableActingMaster(false);
        final BrokerBasicInfo slaveBroker = BrokerBasicInfo.slaveBroker().enableActingMaster(false);

        Channel masterChannel = mock(Channel.class);
        Channel slaveChannel = mock(Channel.class);

        registerBroker(masterBroker, masterChannel, null, "TestTopic");
        registerBroker(slaveBroker, slaveChannel, null, "TestTopic");

        // Master Down
        routeInfoManager.onChannelDestroy(masterChannel);
        await().atMost(Duration.ofSeconds(5)).until(() -> routeInfoManager.blockedUnRegisterRequests() == 0);

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsOnlyKeys(1L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(slaveBroker.brokerAddr);

        // Switch slave to master
        slaveBroker.id(0).dataVersion.nextVersion();
        registerBrokerWithNormalTopic(slaveBroker, "TestTopic");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsOnlyKeys(0L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(slaveBroker.brokerAddr);

        // Old master switch to slave
        masterBroker.id(1).dataVersion.nextVersion();
        registerBrokerWithNormalTopic(masterBroker, "TestTopic");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs()).containsKeys(0L, 1L);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerAddrs())
            .containsValues(BrokerBasicInfo.defaultBroker().brokerAddr, BrokerBasicInfo.slaveBroker().brokerAddr);
    }

    @Test
    public void keepTopicWithBrokerRegistration() {
        RegisterBrokerResult masterResult = registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic", "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNotNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNotNull();

        masterResult = registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(),  "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNotNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNotNull();
    }

    @Test
    public void deleteTopicWithBrokerRegistration() {
        config.setDeleteTopicWithBrokerRegistration(true);
        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(), "TestTopic", "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNotNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNotNull();

        registerBrokerWithNormalTopic(BrokerBasicInfo.defaultBroker(),  "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNotNull();
    }

    @Test
    public void deleteTopicWithBrokerRegistration2() {
        // Register two brokers and delete a specific one by one
        config.setDeleteTopicWithBrokerRegistration(true);
        final BrokerBasicInfo master1 = BrokerBasicInfo.defaultBroker();
        final BrokerBasicInfo master2 = BrokerBasicInfo.defaultBroker().name(DEFAULT_BROKER + 1).addr(DEFAULT_ADDR + 9);

        registerBrokerWithNormalTopic(master1, "TestTopic", "TestTopic1");
        registerBrokerWithNormalTopic(master2, "TestTopic", "TestTopic1");

        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas()).hasSize(2);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1").getBrokerDatas()).hasSize(2);


        registerBrokerWithNormalTopic(master1,  "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas()).hasSize(1);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic").getBrokerDatas().get(0).getBrokerName())
            .isEqualTo(master2.brokerName);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1").getBrokerDatas()).hasSize(2);

        registerBrokerWithNormalTopic(master2,  "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1").getBrokerDatas()).hasSize(2);
    }

    @Test
    public void registerSingleTopicWithBrokerRegistration() {
        config.setDeleteTopicWithBrokerRegistration(true);
        final BrokerBasicInfo master1 = BrokerBasicInfo.defaultBroker();

        registerSingleTopicWithBrokerName(master1.brokerName, "TestTopic");

        // Single topic registration failed because there is no broker connection exists
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNull();

        // Register broker with TestTopic first and then register single topic TestTopic1
        registerBrokerWithNormalTopic(master1, "TestTopic");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNotNull();

        registerSingleTopicWithBrokerName(master1.brokerName, "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNotNull();

        // Register the two topics to keep the route info
        registerBrokerWithNormalTopic(master1, "TestTopic", "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNotNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNotNull();

        // Cancel the TestTopic1 with broker registration
        registerBrokerWithNormalTopic(master1, "TestTopic");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNotNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNull();

        // Add TestTopic1 and cancel all the topics with broker un-registration
        registerSingleTopicWithBrokerName(master1.brokerName, "TestTopic1");
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNotNull();

        routeInfoManager.unregisterBroker(master1.clusterName, master1.brokerAddr, master1.brokerName, 0);
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic")).isNull();
        assertThat(routeInfoManager.pickupTopicRouteData("TestTopic1")).isNull();


    }

    private RegisterBrokerResult registerBrokerWithNormalTopic(BrokerBasicInfo brokerInfo, String... topics) {
        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
        TopicConfig baseTopic = new TopicConfig("baseTopic");
        topicConfigConcurrentHashMap.put(baseTopic.getTopicName(), baseTopic);
        for (final String topic : topics) {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicName(topic);
            topicConfig.setPerm(6);
            topicConfig.setReadQueueNums(8);
            topicConfig.setOrder(false);
            topicConfigConcurrentHashMap.put(topic, topicConfig);
        }

        return registerBroker(brokerInfo, mock(Channel.class), topicConfigConcurrentHashMap, topics);
    }

    private RegisterBrokerResult registerBrokerWithOrderTopic(BrokerBasicInfo brokerBasicInfo, String... topics) {
        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();

        TopicConfig baseTopic = new TopicConfig("baseTopic");
        baseTopic.setOrder(true);
        topicConfigConcurrentHashMap.put(baseTopic.getTopicName(), baseTopic);
        for (final String topic : topics) {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setWriteQueueNums(8);
            topicConfig.setTopicName(topic);
            topicConfig.setPerm(6);
            topicConfig.setReadQueueNums(8);
            topicConfig.setOrder(true);
            topicConfigConcurrentHashMap.put(topic, topicConfig);
        }
        return registerBroker(brokerBasicInfo, mock(Channel.class), topicConfigConcurrentHashMap, topics);
    }

    private RegisterBrokerResult registerBrokerWithGlobalOrderTopic(BrokerBasicInfo brokerBasicInfo, String... topics) {
        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
        TopicConfig baseTopic = new TopicConfig("baseTopic", 1, 1);
        baseTopic.setOrder(true);
        topicConfigConcurrentHashMap.put(baseTopic.getTopicName(), baseTopic);
        for (final String topic : topics) {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setWriteQueueNums(1);
            topicConfig.setTopicName(topic);
            topicConfig.setPerm(6);
            topicConfig.setReadQueueNums(1);
            topicConfig.setOrder(true);
            topicConfigConcurrentHashMap.put(topic, topicConfig);
        }
        return registerBroker(brokerBasicInfo, mock(Channel.class), topicConfigConcurrentHashMap, topics);
    }

    private RegisterBrokerResult registerBroker(BrokerBasicInfo brokerInfo, Channel channel,
        ConcurrentMap<String, TopicConfig> topicConfigConcurrentHashMap, String... topics) {

        if (topicConfigConcurrentHashMap == null) {
            topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
            TopicConfig baseTopic = new TopicConfig("baseTopic");
            topicConfigConcurrentHashMap.put(baseTopic.getTopicName(), baseTopic);
            for (final String topic : topics) {
                TopicConfig topicConfig = new TopicConfig();
                topicConfig.setWriteQueueNums(8);
                topicConfig.setTopicName(topic);
                topicConfig.setPerm(6);
                topicConfig.setReadQueueNums(8);
                topicConfig.setOrder(false);
                topicConfigConcurrentHashMap.put(topic, topicConfig);
            }
        }

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setDataVersion(brokerInfo.dataVersion);
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigConcurrentHashMap);

        RegisterBrokerResult registerBrokerResult = routeInfoManager.registerBroker(
            brokerInfo.clusterName,
            brokerInfo.brokerAddr,
            brokerInfo.brokerName,
            brokerInfo.brokerId,
            brokerInfo.haAddr,
            "",
            null,
            brokerInfo.enableActingMaster,
            topicConfigSerializeWrapper, new ArrayList<>(), channel);
        return registerBrokerResult;
    }

    private void registerSingleTopicWithBrokerName(String brokerName, String... topics) {
        for (final String topic : topics) {
            QueueData queueData = new QueueData();
            queueData.setBrokerName(brokerName);
            queueData.setReadQueueNums(8);
            queueData.setWriteQueueNums(8);
            queueData.setPerm(6);
            routeInfoManager.registerTopic(topic, Collections.singletonList(queueData));
        }
    }

    static class BrokerBasicInfo {
        String clusterName;
        String brokerName;
        String brokerAddr;
        String haAddr;
        int brokerId;
        boolean enableActingMaster;

        DataVersion dataVersion;

        static BrokerBasicInfo defaultBroker() {
            BrokerBasicInfo basicInfo = new BrokerBasicInfo();
            DataVersion dataVersion = new DataVersion();
            dataVersion.setCounter(new AtomicLong(1));
            dataVersion.setTimestamp(System.currentTimeMillis());
            basicInfo.dataVersion = dataVersion;

            return basicInfo.id(0)
                .name(DEFAULT_BROKER)
                .cluster(DEFAULT_CLUSTER)
                .addr(DEFAULT_ADDR)
                .haAddr(DEFAULT_ADDR_PREFIX + "20911")
                .enableActingMaster(true);
        }

        UnRegisterBrokerRequestHeader unRegisterRequest() {
            UnRegisterBrokerRequestHeader unRegisterBrokerRequest = new UnRegisterBrokerRequestHeader();
            unRegisterBrokerRequest.setBrokerAddr(brokerAddr);
            unRegisterBrokerRequest.setBrokerName(brokerName);
            unRegisterBrokerRequest.setClusterName(clusterName);
            unRegisterBrokerRequest.setBrokerId((long) brokerId);
            return unRegisterBrokerRequest;
        }

        static BrokerBasicInfo slaveBroker() {
            final BrokerBasicInfo slaveBroker = defaultBroker();
            return slaveBroker
                .id(1)
                .addr(DEFAULT_ADDR_PREFIX + "30911")
                .haAddr(DEFAULT_ADDR_PREFIX + "40911")
                .enableActingMaster(true);
        }

        BrokerBasicInfo name(String name) {
            this.brokerName = name;
            return this;
        }

        BrokerBasicInfo cluster(String name) {
            this.clusterName = name;
            return this;
        }

        BrokerBasicInfo addr(String addr) {
            this.brokerAddr = addr;
            return this;
        }

        BrokerBasicInfo id(int id) {
            this.brokerId = id;
            return this;
        }

        BrokerBasicInfo haAddr(String addr) {
            this.haAddr = addr;
            return this;
        }

        BrokerBasicInfo enableActingMaster(boolean enableActingMaster) {
            this.enableActingMaster = enableActingMaster;
            return this;
        }
    }

}
